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

import org.apache.mina.common.*;
import org.apache.mina.transport.socket.SocketAcceptor;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Acceptor;
import org.jboss.messaging.core.remoting.CleanUpNotifier;
import org.jboss.messaging.core.remoting.RemotingService;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A Mina TCP Acceptor that supports SSL
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class MinaAcceptor implements Acceptor
{
   private static final Logger log = Logger.getLogger(MinaAcceptor.class);

   private ExecutorService threadPool;
   private SocketAcceptor acceptor;
   private IoServiceListener acceptorListener;
   private CleanUpNotifier cleanupNotifier;
   private RemotingService remotingService;
   private FilterChainSupport chainSupport = new FilterChainSupportImpl();

   public void startAccepting(RemotingService remotingService, CleanUpNotifier cleanupNotifier) throws Exception
   {
      this.remotingService = remotingService;
      this.cleanupNotifier = cleanupNotifier;
      this.acceptor = createAcceptor();

      acceptor.setSessionDataStructureFactory(new MessagingIOSessionDataStructureFactory());

      DefaultIoFilterChainBuilder filterChain = acceptor.getFilterChain();
      
      Configuration config = remotingService.getConfiguration();
      
      log.info(config.getHost());

      // addMDCFilter(filterChain);
      if (config.isSSLEnabled())
      {
         chainSupport.addSSLFilter(filterChain, false, remotingService.getConfiguration().getKeyStorePath(),
                 remotingService.getConfiguration().getKeyStorePassword(), remotingService.getConfiguration()
                 .getTrustStorePath(), remotingService.getConfiguration()
                 .getTrustStorePassword());
      }
      chainSupport.addCodecFilter(filterChain);

      // Bind
      acceptor.setDefaultLocalAddress(new InetSocketAddress(remotingService.getConfiguration().getHost(), remotingService.getConfiguration().getPort()));
      acceptor.getSessionConfig().setTcpNoDelay(remotingService.getConfiguration().getConnectionParams().isTcpNoDelay());
      int receiveBufferSize = remotingService.getConfiguration().getConnectionParams().getTcpReceiveBufferSize();
      if (receiveBufferSize != -1)
      {
         acceptor.getSessionConfig().setReceiveBufferSize(receiveBufferSize);
      }
      int sendBufferSize = remotingService.getConfiguration().getConnectionParams().getTcpSendBufferSize();
      if (sendBufferSize != -1)
      {
         acceptor.getSessionConfig().setSendBufferSize(sendBufferSize);
      }
      acceptor.setReuseAddress(true);
      acceptor.getSessionConfig().setReuseAddress(true);
      acceptor.getSessionConfig().setKeepAlive(true);
      acceptor.setCloseOnDeactivation(false);

      threadPool = Executors.newCachedThreadPool();
      acceptor.setHandler(new MinaHandler(remotingService.getDispatcher(), threadPool, cleanupNotifier, true, true));
      acceptor.bind();
      acceptorListener = new MinaSessionListener();
      acceptor.addListener(acceptorListener);
   }

   public void stopAccepting()
   {
      if (acceptor != null)
      {
         // remove the listener before disposing the acceptor
         // so that we're not notified when the sessions are destroyed
         acceptor.removeListener(acceptorListener);
         acceptor.unbind();
         acceptor.dispose();
         acceptor = null;
         threadPool.shutdown();
      }
   }

   /**
    * This method must only be called by tests which requires
    * to insert Filters (e.g. to simulate network failures)
    */
   public DefaultIoFilterChainBuilder getFilterChain()
   {
      // TODO: get rid of this assert (Validate this condition on tests)
      assert acceptor != null;

      return acceptor.getFilterChain();
   }
   

   // This could be used in Override for tests (to replace an EasyMock for instance)
   protected SocketAcceptor createAcceptor()
   {
      return new NioSocketAcceptor();
   }

   private final class MinaSessionListener implements IoServiceListener
   {

      public void serviceActivated(IoService service)
      {
      }

      public void serviceDeactivated(IoService service)
      {
      }

      public void serviceIdle(IoService service, IdleStatus idleStatus)
      {
      }

      /**
       * register a pinger for the new client
       *
       * @param session
       */
      public void sessionCreated(IoSession session)
      {
         log.info("session id " + session.getId());
         //register pinger
         if (remotingService.getConfiguration().getConnectionParams().getPingInterval() > 0)
         {
            remotingService.registerPinger(new MinaSession(session));
         }
      }

      /**
       * unregister the pinger
       *
       * @param session
       */
      public void sessionDestroyed(IoSession session)
      {
         remotingService.unregisterPinger(session.getId());
         cleanupNotifier.fireCleanup(session.getId(), null);
      }
   }

}
