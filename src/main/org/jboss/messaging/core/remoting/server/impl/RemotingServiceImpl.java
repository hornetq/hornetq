/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors by
 * the @authors tag. See the copyright.txt in the distribution for a full listing of individual contributors. This is
 * free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public License along with this software; if not,
 * write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.core.remoting.server.impl;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.DISCONNECT;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.ChannelHandler;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.AbstractBufferHandler;
import org.jboss.messaging.core.remoting.impl.RemotingConnectionImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.server.RemotingService;
import org.jboss.messaging.core.remoting.spi.Acceptor;
import org.jboss.messaging.core.remoting.spi.AcceptorFactory;
import org.jboss.messaging.core.remoting.spi.BufferHandler;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.impl.MessagingServerPacketHandler;
import org.jboss.messaging.utils.UUIDGenerator;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class RemotingServiceImpl implements RemotingService, ConnectionLifeCycleListener
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(RemotingServiceImpl.class);

   // Attributes ----------------------------------------------------

   private volatile boolean started = false;

   private final Set<TransportConfiguration> transportConfigs;

   private final List<Interceptor> interceptors = new ArrayList<Interceptor>();

   private final Set<Acceptor> acceptors = new HashSet<Acceptor>();

   private final Map<Object, RemotingConnection> connections = new ConcurrentHashMap<Object, RemotingConnection>();

   private Timer failedConnectionTimer;

   private TimerTask failedConnectionsTask;

   private final BufferHandler bufferHandler = new DelegatingBufferHandler();
   
   private final Configuration config;

   private volatile MessagingServer server;

   private ManagementService managementService;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public RemotingServiceImpl(final Configuration config)
   {
      transportConfigs = config.getAcceptorConfigurations();

      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      for (String interceptorClass : config.getInterceptorClassNames())
      {
         try
         {
            Class<?> clazz = loader.loadClass(interceptorClass);
            interceptors.add((Interceptor)clazz.newInstance());
         }
         catch (Exception e)
         {
            log.warn("Error instantiating interceptor \"" + interceptorClass + "\"", e);
         }
      }

      this.config = config;
   }

   // RemotingService implementation -------------------------------

   public void setManagementService(final ManagementService managementService)
   {
      this.managementService = managementService;
   }

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      // when JMX is enabled, it requires a INVM acceptor to send the core messages
      // corresponding to the JMX management operations (@see ReplicationAwareStandardMBeanWrapper)
      if (config.isJMXManagementEnabled())
      {
         boolean invmAcceptorConfigured = false;
         for (TransportConfiguration config : transportConfigs)
         {
            if (InVMAcceptorFactory.class.getName().equals(config.getFactoryClassName()))
            {
               invmAcceptorConfigured = true;
            }
         }
         if (!invmAcceptorConfigured)
         {
            transportConfigs.add(new TransportConfiguration(InVMAcceptorFactory.class.getName(),
                                                            new HashMap<String, Object>(),
                                                            "in-vm"));
         }
      }

      ClassLoader loader = Thread.currentThread().getContextClassLoader();

      for (TransportConfiguration info : transportConfigs)
      {
         try
         {
            Class<?> clazz = loader.loadClass(info.getFactoryClassName());

            AcceptorFactory factory = (AcceptorFactory)clazz.newInstance();
            
            Acceptor acceptor = factory.createAcceptor(info.getParams(), bufferHandler, this);

            acceptors.add(acceptor);

            if (managementService != null)
            {
               managementService.registerAcceptor(acceptor, info);
            }
         }
         catch (Exception e)
         {
            log.warn("Error instantiating acceptor \"" + info.getFactoryClassName() + "\"", e);
         }
      }

      for (Acceptor a : acceptors)
      {
         a.start();
      }

      failedConnectionTimer = new Timer(true);

      failedConnectionsTask = new FailedConnectionsTask();
      
      failedConnectionTimer.schedule(failedConnectionsTask, config.getConnectionScanPeriod(), config.getConnectionScanPeriod());

      started = true;
   }
   
   public synchronized void stop() throws Exception
   {
      if (!started)
      {
         return;
      }

      if (failedConnectionTimer != null)
      {
         failedConnectionsTask.cancel();

         failedConnectionsTask = null;

         failedConnectionTimer.cancel();

         failedConnectionTimer = null;
      }
      
      //We need to stop them accepting first so no new connections are accepted after we send the disconnect message
      for (Acceptor acceptor : acceptors)
      {
         acceptor.pause();
      }
      
      for (RemotingConnection connection: connections.values())
      {                    
         connection.getChannel(0, -1, false).sendAndFlush(new PacketImpl(DISCONNECT));
      }

      for (Acceptor acceptor : acceptors)
      {
         acceptor.stop();
      }

      acceptors.clear();

      connections.clear();

      started = false;
   }

   public boolean isStarted()
   {
      return started;
   }

   public RemotingConnection getConnection(final Object remotingConnectionID)
   {
      return connections.get(remotingConnectionID);
   }

   public synchronized Set<RemotingConnection> getConnections()
   {
      return new HashSet<RemotingConnection>(connections.values());
   }

   public void setMessagingServer(final MessagingServer server)
   {
      this.server = server;
   }

   // ConnectionLifeCycleListener implementation -----------------------------------

   public void connectionCreated(final Connection connection)
   {
      if (server == null)
      {
         throw new IllegalStateException("Unable to create connection, server hasn't finished starting up");
      }

      RemotingConnection rc = new RemotingConnectionImpl(connection, interceptors, !config.isBackup(), config.getConnectionTTLOverride());

      Channel channel1 = rc.getChannel(1, -1, false);

      ChannelHandler handler = new MessagingServerPacketHandler(server, channel1, rc);

      channel1.setHandler(handler);

      Object id = connection.getID();

      connections.put(id, rc);
   }

   public void connectionDestroyed(final Object connectionID)
   {  
      RemotingConnection conn = connections.remove(connectionID);
      if (conn != null)
      {
         conn.destroy();
      }      
   }

   public void connectionException(final Object connectionID, final MessagingException me)
   {
      RemotingConnection rc = connections.remove(connectionID);

      if (rc != null)
      {
         rc.fail(me);
      }     
   }

   public void addInterceptor(final Interceptor interceptor)
   {
      interceptors.add(interceptor);
   }

   public boolean removeInterceptor(final Interceptor interceptor)
   {
      return interceptors.remove(interceptor);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private class FailedConnectionsTask extends TimerTask
   {
      private boolean cancelled;

      @Override
      public synchronized void run()
      {
         if (cancelled)
         {
            return;
         }

         Set<RemotingConnection> failedConnections = new HashSet<RemotingConnection>();

         long now = System.currentTimeMillis();

         for (RemotingConnection conn : connections.values())
         {
            if (conn.isExpired(now))
            {
               failedConnections.add(conn);
            } 
         }

         for (RemotingConnection conn : failedConnections)
         {
            MessagingException me = new MessagingException(MessagingException.CONNECTION_TIMEDOUT,
                                                           "Did not receive ping on connection. It is likely a client has exited or crashed without " + "closing its connection, or the network between the server and client has failed. The connection will now be closed.");

            conn.fail(me);
         }
      }

      @Override
      public synchronized boolean cancel()
      {
         cancelled = true;

         return super.cancel();
      }
   }

   private class DelegatingBufferHandler extends AbstractBufferHandler
   {
      public void bufferReceived(final Object connectionID, final MessagingBuffer buffer)
      {         
         RemotingConnection conn = connections.get(connectionID);

         if (conn != null)
         {
            conn.bufferReceived(connectionID, buffer);
         }
      }
   }

}