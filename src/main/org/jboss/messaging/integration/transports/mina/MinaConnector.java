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
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.service.IoService;
import org.apache.mina.core.service.IoServiceListener;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.transport.socket.SocketConnector;
import org.apache.mina.transport.socket.SocketSessionConfig;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.spi.BufferHandler;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.spi.Connector;
import org.jboss.messaging.utils.ConfigurationHelper;

/**
 *
 * A MinaConnector
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class MinaConnector implements Connector
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(MinaConnector.class);

   // Attributes ----------------------------------------------------

   private SocketConnector connector;

   private final BufferHandler handler;

   private final ConnectionLifeCycleListener listener;
   
   private final boolean sslEnabled;
   
   private final String host;

   private final int port;
         
   private final String keyStorePath;
 
   private final String keyStorePassword;
   
   private final boolean tcpNoDelay;
   
   private final int tcpSendBufferSize;
   
   private final int tcpReceiveBufferSize;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public MinaConnector(final Map<String, Object> configuration,                 
                        final BufferHandler handler,
                        final ConnectionLifeCycleListener listener)
   {
      if (listener == null)
      {
         throw new IllegalArgumentException("Invalid argument null listener");
      }
      
      if (handler == null)
      {
         throw new IllegalArgumentException("Invalid argument null handler");
      }

      this.listener = listener;
      
      this.handler = handler;
      
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
      }   
      else
      {
         this.keyStorePath = null;
         this.keyStorePassword = null;
      }
      
      this.tcpNoDelay =
         ConfigurationHelper.getBooleanProperty(TransportConstants.TCP_NODELAY_PROPNAME, TransportConstants.DEFAULT_TCP_NODELAY, configuration);
      this.tcpSendBufferSize =
         ConfigurationHelper.getIntProperty(TransportConstants.TCP_SENDBUFFER_SIZE_PROPNAME, TransportConstants.DEFAULT_TCP_SENDBUFFER_SIZE, configuration);
      this.tcpReceiveBufferSize =
         ConfigurationHelper.getIntProperty(TransportConstants.TCP_RECEIVEBUFFER_SIZE_PROPNAME, TransportConstants.DEFAULT_TCP_RECEIVEBUFFER_SIZE, configuration);

   }

   public synchronized void start()
   {
      if (connector != null)
      {
         return;
      }

      connector = new NioSocketConnector();

      SocketSessionConfig connectorConfig = connector.getSessionConfig();

      DefaultIoFilterChainBuilder filterChain = connector.getFilterChain();

      connector.setSessionDataStructureFactory(new MessagingIOSessionDataStructureFactory());

      connectorConfig.setTcpNoDelay(tcpNoDelay);
      if (tcpReceiveBufferSize != -1)
      {
         connectorConfig.setReceiveBufferSize(tcpReceiveBufferSize);
      }
      if (tcpSendBufferSize != -1)
      {
         connectorConfig.setSendBufferSize(tcpSendBufferSize);
      }
      connectorConfig.setKeepAlive(true);
      connectorConfig.setReuseAddress(true);

      if (this.sslEnabled)
      {
         try
         {
            FilterChainSupport.addSSLFilter(filterChain, true, keyStorePath, keyStorePassword, null, null);
         }
         catch (Exception e)
         {
            IllegalStateException ise = new IllegalStateException("Unable to create MinaConnection for " + host);
            ise.initCause(e);
            throw ise;
         }
      }
      FilterChainSupport.addCodecFilter(filterChain, handler);

      connector.setHandler(new MinaHandler());

      connector.addListener(new ServiceListener());
   }
   
   public synchronized void close()
   {
      if (connector != null)
      {
         connector.dispose();
         connector = null;
      }
   }
   
   public boolean isStarted()
   {
      return (connector != null);
   }

   public Connection createConnection()
   {
      InetSocketAddress address = new InetSocketAddress(host, port);
      ConnectFuture future = connector.connect(address);
      connector.setDefaultRemoteAddress(address);

      future.awaitUninterruptibly();

      if (future.isConnected())
      {
         IoSession session = future.getSession();

         return new MinaConnection(session);
      }
      else
      {
         return null;
      }
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private final class ServiceListener implements IoServiceListener
   {
      private ServiceListener()
      {
      }

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
         listener.connectionDestroyed(session.getId());
      }
   }

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

}
