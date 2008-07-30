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

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.filterchain.DefaultIoFilterChainBuilder;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.service.IoService;
import org.apache.mina.core.service.IoServiceListener;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.transport.socket.SocketAcceptor;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.RemotingHandler;
import org.jboss.messaging.core.remoting.spi.Acceptor;
import org.jboss.messaging.core.remoting.spi.Connection;

/**
 * A Mina TCP Acceptor that supports SSL
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class MinaAcceptor implements Acceptor
{
   private static final Logger log = Logger.getLogger(MinaAcceptor.class);

   private SocketAcceptor acceptor;

   private IoServiceListener acceptorListener;

   private final Configuration configuration;

   private final RemotingHandler handler;

   private final ConnectionLifeCycleListener listener;

   public MinaAcceptor(final Configuration configuration, final RemotingHandler handler,
                       final ConnectionLifeCycleListener listener)
   {
      this.configuration = configuration;

      this.handler = handler;

      this.listener = listener;
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

      if (configuration.isSSLEnabled())
      {
         FilterChainSupport.addSSLFilter(filterChain, false, configuration.getKeyStorePath(),
                 configuration.getKeyStorePassword(),
                 configuration.getTrustStorePath(),
                 configuration.getTrustStorePassword());
      }
      FilterChainSupport.addCodecFilter(filterChain, handler);

      // Bind
      acceptor.setDefaultLocalAddress(new InetSocketAddress(configuration.getHost(), configuration.getPort()));
      acceptor.getSessionConfig().setTcpNoDelay(configuration.getConnectionParams().isTcpNoDelay());
      int receiveBufferSize = configuration.getConnectionParams().getTcpReceiveBufferSize();
      if (receiveBufferSize != -1)
      {
         acceptor.getSessionConfig().setReceiveBufferSize(receiveBufferSize);
      }
      int sendBufferSize = configuration.getConnectionParams().getTcpSendBufferSize();
      if (sendBufferSize != -1)
      {
         acceptor.getSessionConfig().setSendBufferSize(sendBufferSize);
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
