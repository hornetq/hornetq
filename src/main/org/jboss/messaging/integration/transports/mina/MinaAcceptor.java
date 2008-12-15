/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.messaging.integration.transports.mina;

import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.filterchain.DefaultIoFilterChainBuilder;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.service.IoService;
import org.apache.mina.core.service.IoServiceListener;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.transport.socket.SocketAcceptor;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.spi.Acceptor;
import org.jboss.messaging.core.remoting.spi.BufferHandler;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.util.ConfigurationHelper;


/**
 * A Mina TCP Acceptor that supports SSL
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class MinaAcceptor implements Acceptor
{
   public static final Logger log = Logger.getLogger(MinaAcceptor.class);
   
   
   // Attributes ------------------------------------------------------------------------------------

   private SocketAcceptor acceptor;

   private IoServiceListener acceptorListener;

   private final BufferHandler handler;

   private final ConnectionLifeCycleListener listener;
   
   private final boolean sslEnabled;
   
   private final String host;

   private final int port;
         
   private final String keyStorePath;
 
   private final String keyStorePassword;
   
   private final String trustStorePath;
   
   private final String trustStorePassword;
   
   private final boolean tcpNoDelay;
   
   private final int tcpSendBufferSize;
   
   private final int tcpReceiveBufferSize;

   public MinaAcceptor(final Map<String, Object> configuration, final BufferHandler handler,
                       final ConnectionLifeCycleListener listener)
   {
      this.handler = handler;

      this.listener = listener;
      
      this.sslEnabled =
         ConfigurationHelper.getBooleanProperty(TransportConstants.SSL_ENABLED_PROP_NAME, TransportConstants.DEFAULT_SSL_ENABLED, configuration);
      this.host =
         ConfigurationHelper.getStringProperty(TransportConstants.HOST_PROP_NAME, TransportConstants.DEFAULT_HOST, configuration);
      this.port =
         ConfigurationHelper.getIntProperty(TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_PORT, configuration);
      if (sslEnabled)
      {
         this.keyStorePath =
            ConfigurationHelper.getStringProperty(TransportConstants.KEYSTORE_PATH_PROP_NAME, TransportConstants.DEFAULT_KEYSTORE_PATH, configuration);
         this.keyStorePassword =
            ConfigurationHelper.getStringProperty(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, TransportConstants.DEFAULT_KEYSTORE_PASSWORD, configuration);
         this.trustStorePath =
            ConfigurationHelper.getStringProperty(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, TransportConstants.DEFAULT_TRUSTSTORE_PATH, configuration);
         this.trustStorePassword =
            ConfigurationHelper.getStringProperty(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, TransportConstants.DEFAULT_TRUSTSTORE_PASSWORD, configuration);
      }   
      else
      {
         this.keyStorePath = null;
         this.keyStorePassword = null;
         this.trustStorePath = null;
         this.trustStorePassword = null; 
      }
      
      this.tcpNoDelay =
         ConfigurationHelper.getBooleanProperty(TransportConstants.TCP_NODELAY_PROPNAME, TransportConstants.DEFAULT_TCP_NODELAY, configuration);
      this.tcpSendBufferSize =
         ConfigurationHelper.getIntProperty(TransportConstants.TCP_SENDBUFFER_SIZE_PROPNAME, TransportConstants.DEFAULT_TCP_SENDBUFFER_SIZE, configuration);
      this.tcpReceiveBufferSize =
         ConfigurationHelper.getIntProperty(TransportConstants.TCP_RECEIVEBUFFER_SIZE_PROPNAME, TransportConstants.DEFAULT_TCP_RECEIVEBUFFER_SIZE, configuration);
     
   }

   public synchronized void start() throws Exception
   {
      if (acceptor != null)
      {
         //Already started
         return;
      }

      acceptor = new NioSocketAcceptor();

      acceptor.setSessionDataStructureFactory(new MessagingIOSessionDataStructureFactory());

      DefaultIoFilterChainBuilder filterChain = acceptor.getFilterChain();

      if (sslEnabled)
      {
         FilterChainSupport.addSSLFilter(filterChain, false, keyStorePath,
                 keyStorePassword,
                 trustStorePath,
                 trustStorePassword);
      }
      FilterChainSupport.addCodecFilter(filterChain, handler);

      // Bind
      acceptor.setDefaultLocalAddress(new InetSocketAddress(host, port));      
      acceptor.getSessionConfig().setTcpNoDelay(tcpNoDelay);      
      if (tcpReceiveBufferSize != -1)
      {
         acceptor.getSessionConfig().setReceiveBufferSize(tcpReceiveBufferSize);
      }     
      if (tcpSendBufferSize != -1)
      {
         acceptor.getSessionConfig().setSendBufferSize(tcpSendBufferSize);
      }
      acceptor.setReuseAddress(true);
      acceptor.getSessionConfig().setReuseAddress(true);
      acceptor.getSessionConfig().setKeepAlive(true);
      acceptor.setCloseOnDeactivation(false);

      acceptor.setHandler(new MinaHandler());
      acceptor.bind();
      acceptorListener = new MinaSessionListener();
      acceptor.addListener(acceptorListener);
   }

   public synchronized void stop()
   {
      if (acceptor == null)
      {
         return;
      }

      // remove the listener before disposing the acceptor
      // so that we're not notified when the sessions are destroyed
      acceptor.removeListener(acceptorListener);
      acceptor.unbind();
      acceptor.dispose();
      acceptor = null;
   }

   public boolean isStarted()
   {
      return (acceptor != null);
   }
   
   public DefaultIoFilterChainBuilder getFilterChain()
   {
      return acceptor.getFilterChain();
   }

   // Inner classes -----------------------------------------------------------------------------

   private final class MinaHandler extends IoHandlerAdapter
   {
      @Override
      public void exceptionCaught(final IoSession session, final Throwable cause)
              throws Exception
      {
         log.error("caught exception " + cause + " for session " + session, cause);

         MessagingException me = new MessagingException(MessagingException.INTERNAL_ERROR, "MINA exception");

         me.initCause(cause);

         listener.connectionException(session.getId(), me);
      }

      @Override
      public void messageReceived(final IoSession session, final Object message)
              throws Exception
      {
         IoBuffer buffer = (IoBuffer) message;

         handler.bufferReceived(session.getId(), new IoBufferWrapper(buffer));
      }
   }

   private final class MinaSessionListener implements IoServiceListener
   {

      public void serviceActivated(final IoService service)
      {
      }

      public void serviceDeactivated(final IoService service)
      {
      }

      public void serviceIdle(final IoService service, final IdleStatus idleStatus)
      {
      }

      public void sessionCreated(final IoSession session)
      {
         Connection tc = new MinaConnection(session);

         listener.connectionCreated(tc);
      }

      public void sessionDestroyed(final IoSession session)
      {
         listener.connectionDestroyed(session.getId());
      }
   }

}
