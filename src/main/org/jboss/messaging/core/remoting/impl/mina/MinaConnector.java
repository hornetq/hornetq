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
package org.jboss.messaging.core.remoting.impl.mina;

import java.net.InetSocketAddress;

import org.apache.mina.common.ConnectFuture;
import org.apache.mina.common.DefaultIoFilterChainBuilder;
import org.apache.mina.common.IdleStatus;
import org.apache.mina.common.IoBuffer;
import org.apache.mina.common.IoHandlerAdapter;
import org.apache.mina.common.IoService;
import org.apache.mina.common.IoServiceListener;
import org.apache.mina.common.IoSession;
import org.apache.mina.transport.socket.SocketConnector;
import org.apache.mina.transport.socket.SocketSessionConfig;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.RemotingHandler;
import org.jboss.messaging.core.remoting.spi.Connector;
import org.jboss.messaging.core.remoting.spi.Connection;

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

   private static final Logger log = Logger.getLogger(MinaConnection.class);

   // Attributes ----------------------------------------------------

   private SocketConnector connector;
   
   private final RemotingHandler handler;
   
   private final Location location;
   
   private final ConnectionLifeCycleListener listener;
   
   private final ConnectionParams params;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   
   public MinaConnector(final Location location, final ConnectionParams params,
                        final RemotingHandler handler,
                        final ConnectionLifeCycleListener listener)
   {            
      if (location == null)
      {
         throw new IllegalArgumentException("Invalid argument null location");
      }
      
      if (params == null)
      {
         throw new IllegalArgumentException("Invalid argument null connection params");
      }
      
      if (handler == null)
      {
         throw new IllegalArgumentException("Invalid argument null handler");
      }
      
      if (listener == null)
      {
         throw new IllegalArgumentException("Invalid argument null listener");
      }
      
      this.handler = handler;      
      this.location = location;
      this.listener = listener;
      this.params = params;            
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

      FilterChainSupport.addCodecFilter(filterChain, handler);
      
      connectorConfig.setTcpNoDelay(params.isTcpNoDelay());
      if (params.getTcpReceiveBufferSize() != -1)
      {
         connectorConfig.setReceiveBufferSize(params.getTcpReceiveBufferSize());
      }      
      if (params.getTcpSendBufferSize() != -1)
      {
         connectorConfig.setSendBufferSize(params.getTcpSendBufferSize());
      }
      connectorConfig.setKeepAlive(true);
      connectorConfig.setReuseAddress(true);

      if (params.isSSLEnabled())
      {    
         try
         {
            FilterChainSupport.addSSLFilter(filterChain, true, params.getKeyStorePath(), params.getKeyStorePassword(), null, null);
         }
         catch (Exception e)
         {
            IllegalStateException ise = new IllegalStateException("Unable to create MinaConnection for " + location);
            ise.initCause(e);
            throw ise;
         }
      }

      
      connector.setHandler(new MinaHandler());   
      
      connector.addListener(new ServiceListener());
   }
   
   public synchronized void close()
   {
      if (connector != null)
      {     
         connector.dispose();
      }            
   }
   
   public Connection createConnection()
   {
      InetSocketAddress address = new InetSocketAddress(location.getHost(), location.getPort());
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
      public void exceptionCaught(final IoSession session, final Throwable cause)
              throws Exception
      {
         log.error("caught exception " + cause + " for session " + session, cause);

         MessagingException me = new MessagingException(MessagingException.INTERNAL_ERROR, "MINA exception");
         
         me.initCause(cause);
         
         listener.connectionException(session.getId(), me);  
      }

      public void messageReceived(final IoSession session, final Object message)
              throws Exception
      {
         IoBuffer buffer = (IoBuffer) message;
         
         handler.bufferReceived(session.getId(), new IoBufferWrapper(buffer));
      }
   }
   
}
