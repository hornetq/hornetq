/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.integration;

import java.net.InetSocketAddress;
import java.util.Formatter;

import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.filter.logging.MdcInjectionFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.remoting.PacketDispatcher;

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

   private final int port;

   private final String host;
   
   private NioSocketAcceptor acceptor;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------


   public MinaService(String host, int port)
   {
      assert host !=  null;
      assert port > 0;
      
      this.host = host;
      this.port = port;
   }

   // Public --------------------------------------------------------
   
   public int getPort()
   {
      return port;
   }
      
   public String getHost()
   {
     return host;
   }
   
   public void start() throws Exception
   {
      if (acceptor == null)
      {
         info("Starting MINA on " + host + ":" + port);

         acceptor = new NioSocketAcceptor();

         // Prepare the configuration
         MdcInjectionFilter mdcInjectionFilter = new MdcInjectionFilter();
         acceptor.getFilterChain().addLast("mdc", mdcInjectionFilter);
         acceptor.getFilterChain().addLast("codec",
               new ProtocolCodecFilter(new PacketCodecFactory()));
         acceptor.getFilterChain().addLast("logger", new LoggingFilter());

         // Bind
         acceptor.setLocalAddress(new InetSocketAddress(host, port));
         acceptor.setReuseAddress(true);
         acceptor.getSessionConfig().setReuseAddress(true);
         acceptor.getSessionConfig().setKeepAlive(true);
         acceptor.setDisconnectOnUnbind(false);

         acceptor.setHandler(new MinaHandler(PacketDispatcher.server));
         acceptor.bind();

         info("MINA started");
      } 
   }
   

   public void stop()
   {
      if (acceptor != null)
      {
         acceptor.unbind();
         acceptor.dispose();
         acceptor = null;
         
         info("Stopped MINA ");
      }    
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   
   private void info(String s)
   {
      log.info(new Formatter().format("### %-30s ###", s).toString());
   }
   
   // Inner classes -------------------------------------------------
}
