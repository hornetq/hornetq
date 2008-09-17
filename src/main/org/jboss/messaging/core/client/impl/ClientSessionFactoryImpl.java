/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat
 * Middleware LLC, and individual contributors by the @authors tag. See the
 * copyright.txt in the distribution for a full listing of individual
 * contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */
package org.jboss.messaging.core.client.impl;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.EXCEPTION;

import java.util.Map;
import java.util.Set;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.ChannelHandler;
import org.jboss.messaging.core.remoting.ConnectionRegistry;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.ConnectionRegistryImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.spi.ConnectorFactory;
import org.jboss.messaging.core.version.Version;
import org.jboss.messaging.util.ConcurrentHashSet;
import org.jboss.messaging.util.UUIDGenerator;
import org.jboss.messaging.util.VersionLoader;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 *
 * @version <tt>$Revision: 3602 $</tt>
 *
 */
public class ClientSessionFactoryImpl implements ClientSessionFactoryInternal, FailureListener
{
   // Constants
   // ------------------------------------------------------------------------------------

   private static final long serialVersionUID = 2512460695662741413L;

   private static final Logger log = Logger.getLogger(ClientSessionFactoryImpl.class);

   public static final long DEFAULT_PING_PERIOD = 2000;

   public static final long DEFAULT_CALL_TIMEOUT = 30000;

   public static final int DEFAULT_CONSUMER_WINDOW_SIZE = 1024 * 1024;

   public static final int DEFAULT_CONSUMER_MAX_RATE = -1;

   public static final int DEFAULT_PRODUCER_WINDOW_SIZE = 1024 * 1024;

   public static final int DEFAULT_PRODUCER_MAX_RATE = -1;

   public static final boolean DEFAULT_BLOCK_ON_ACKNOWLEDGE = false;

   public static final boolean DEFAULT_BLOCK_ON_PERSISTENT_SEND = false;

   public static final boolean DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND = false;

   // Attributes
   // -----------------------------------------------------------------------------------

   private ConnectionRegistry connectionRegistry;

   // These attributes are mutable and can be updated by different threads so
   // must be volatile

   private volatile ConnectorFactory connectorFactory;

   private volatile Map<String, Object> transportParams;

   private volatile ConnectorFactory backupConnectorFactory;

   private volatile Map<String, Object> backupTransportParams;

   private volatile long pingPeriod;

   private volatile long callTimeout;

   private volatile int consumerWindowSize;

   private volatile int consumerMaxRate;

   private volatile int producerWindowSize;

   private volatile int producerMaxRate;

   private volatile boolean blockOnAcknowledge;

   private volatile boolean blockOnPersistentSend;

   private volatile boolean blockOnNonPersistentSend;

   private volatile boolean failedOver;

   private final Set<ClientSessionInternal> sessions = new ConcurrentHashSet<ClientSessionInternal>();

   // Static
   // ---------------------------------------------------------------------------------------

   // Constructors
   // ---------------------------------------------------------------------------------

   /**
    * Create a ClientSessionFactoryImpl specifying all attributes
    */
   public ClientSessionFactoryImpl(final TransportConfiguration connectorConfig,
                                   final TransportConfiguration backupConfig,
                                   final long pingPeriod,
                                   final long callTimeout,
                                   final int consumerWindowSize,
                                   final int consumerMaxRate,
                                   final int producerWindowSize,
                                   final int producerMaxRate,
                                   final boolean blockOnAcknowledge,
                                   final boolean blockOnNonPersistentSend,
                                   final boolean blockOnPersistentSend)
   {
      this.connectorFactory = instantiateConnectorFactory(connectorConfig.getFactoryClassName());
      this.transportParams = connectorConfig.getParams();
      if (backupConfig != null)
      {
         this.backupConnectorFactory = instantiateConnectorFactory(backupConfig.getFactoryClassName());
         this.backupTransportParams = backupConfig.getParams();
      }
      this.pingPeriod = pingPeriod;
      this.callTimeout = callTimeout;
      this.consumerWindowSize = consumerWindowSize;
      this.consumerMaxRate = consumerMaxRate;
      this.producerWindowSize = producerWindowSize;
      this.producerMaxRate = producerMaxRate;
      this.blockOnAcknowledge = blockOnAcknowledge;
      this.blockOnNonPersistentSend = blockOnNonPersistentSend;
      this.blockOnPersistentSend = blockOnPersistentSend;
      this.connectionRegistry = ConnectionRegistryImpl.instance;
   }

   public ClientSessionFactoryImpl(final TransportConfiguration connectorConfig,
                                   final TransportConfiguration backupConfig)
   {
      this.connectorFactory = instantiateConnectorFactory(connectorConfig.getFactoryClassName());
      this.transportParams = connectorConfig.getParams();
      if (backupConfig != null)
      {
         this.backupConnectorFactory = instantiateConnectorFactory(backupConfig.getFactoryClassName());
         this.backupTransportParams = backupConfig.getParams();
      }
      pingPeriod = DEFAULT_PING_PERIOD;
      callTimeout = DEFAULT_CALL_TIMEOUT;
      consumerWindowSize = DEFAULT_CONSUMER_WINDOW_SIZE;
      consumerMaxRate = DEFAULT_CONSUMER_MAX_RATE;
      producerWindowSize = DEFAULT_PRODUCER_WINDOW_SIZE;
      producerMaxRate = DEFAULT_PRODUCER_MAX_RATE;
      blockOnAcknowledge = DEFAULT_BLOCK_ON_ACKNOWLEDGE;
      blockOnPersistentSend = DEFAULT_BLOCK_ON_PERSISTENT_SEND;
      blockOnNonPersistentSend = DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND;
      this.connectionRegistry = ConnectionRegistryImpl.instance;
   }

   /**
    * Create a ClientSessionFactoryImpl specify transport type and using defaults
    */
   public ClientSessionFactoryImpl(final TransportConfiguration connectorConfig)
   {
      this.connectorFactory = instantiateConnectorFactory(connectorConfig.getFactoryClassName());
      this.transportParams = connectorConfig.getParams();
      pingPeriod = DEFAULT_PING_PERIOD;
      callTimeout = DEFAULT_CALL_TIMEOUT;
      consumerWindowSize = DEFAULT_CONSUMER_WINDOW_SIZE;
      consumerMaxRate = DEFAULT_CONSUMER_MAX_RATE;
      producerWindowSize = DEFAULT_PRODUCER_WINDOW_SIZE;
      producerMaxRate = DEFAULT_PRODUCER_MAX_RATE;
      blockOnAcknowledge = DEFAULT_BLOCK_ON_ACKNOWLEDGE;
      blockOnPersistentSend = DEFAULT_BLOCK_ON_PERSISTENT_SEND;
      blockOnNonPersistentSend = DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND;
      this.connectionRegistry = ConnectionRegistryImpl.instance;
   }

   // ClientSessionFactory implementation
   // ---------------------------------------------

   public ClientSession createSession(final String username,
                                      final String password,
                                      final boolean xa,
                                      final boolean autoCommitSends,
                                      final boolean autoCommitAcks,
                                      int lazyAckBatchSize,
                                      boolean cacheProducers) throws MessagingException
   {
      return createSessionInternal(username,
                                   password,
                                   xa,
                                   autoCommitSends,
                                   autoCommitAcks,
                                   lazyAckBatchSize,
                                   cacheProducers);
   }

   public ClientSession createSession(final boolean xa,
                                      final boolean autoCommitSends,
                                      final boolean autoCommitAcks,
                                      int lazyAckBatchSize,
                                      boolean cacheProducers) throws MessagingException
   {
      return createSessionInternal(null, null, xa, autoCommitSends, autoCommitAcks, lazyAckBatchSize, cacheProducers);
   }

   public int getConsumerWindowSize()
   {
      return consumerWindowSize;
   }

   public void setConsumerWindowSize(final int size)
   {
      consumerWindowSize = size;
   }

   public int getProducerWindowSize()
   {
      return producerWindowSize;
   }

   public void setProducerWindowSize(final int size)
   {
      producerWindowSize = size;
   }

   public int getProducerMaxRate()
   {
      return producerMaxRate;
   }

   public void setProducerMaxRate(final int rate)
   {
      this.producerMaxRate = rate;
   }

   public int getConsumerMaxRate()
   {
      return consumerMaxRate;
   }

   public void setConsumerMaxRate(final int rate)
   {
      this.consumerMaxRate = rate;
   }

   public boolean isBlockOnPersistentSend()
   {
      return blockOnPersistentSend;
   }

   public void setBlockOnPersistentSend(final boolean blocking)
   {
      blockOnPersistentSend = blocking;
   }

   public boolean isBlockOnNonPersistentSend()
   {
      return blockOnNonPersistentSend;
   }

   public void setBlockOnNonPersistentSend(final boolean blocking)
   {
      blockOnNonPersistentSend = blocking;
   }

   public boolean isBlockOnAcknowledge()
   {
      return this.blockOnAcknowledge;
   }

   public void setBlockOnAcknowledge(final boolean blocking)
   {
      blockOnAcknowledge = blocking;
   }

   public ConnectorFactory getConnectorFactory()
   {
      return connectorFactory;
   }

   public void setConnectorFactory(final ConnectorFactory connectorFactory)
   {
      if (!sessions.isEmpty())
      {
         throw new IllegalStateException("Cannot set connector factory after connections have been created");
      }

      this.connectorFactory = connectorFactory;
   }

   public Map<String, Object> getTransportParams()
   {
      return transportParams;
   }

   public void setTransportParams(final Map<String, Object> transportParams)
   {
      if (!sessions.isEmpty())
      {
         throw new IllegalStateException("Cannot set transport params after connections have been created");
      }

      this.transportParams = transportParams;
   }

   public ConnectorFactory getBackupConnectorFactory()
   {
      return backupConnectorFactory;
   }

   public void setBackupConnectorFactory(final ConnectorFactory connectorFactory)
   {
      if (!sessions.isEmpty())
      {
         throw new IllegalStateException("Cannot set backup connector factory after connections have been created");
      }

      this.backupConnectorFactory = connectorFactory;
   }

   public Map<String, Object> getBackupTransportParams()
   {
      return backupTransportParams;
   }

   public void setBackupTransportParams(final Map<String, Object> transportParams)
   {
      if (!sessions.isEmpty())
      {
         throw new IllegalStateException("Cannot set backup transport params after connections have been created");
      }

      this.backupTransportParams = transportParams;
   }

   public long getPingPeriod()
   {
      return pingPeriod;
   }

   public void setPingPeriod(final long pingPeriod)
   {
      this.pingPeriod = pingPeriod;
   }

   public long getCallTimeout()
   {
      return callTimeout;
   }

   public void setCallTimeout(final long callTimeout)
   {
      this.callTimeout = callTimeout;
   }

   public boolean isFailedOver()
   {
      return failedOver;
   }

   // ClientSessionFactoryInternal implementation
   // ------------------------------------------

   public void removeSession(final ClientSessionInternal session)
   {
      sessions.remove(session);
   }

   // Public
   // ---------------------------------------------------------------------------------------

   public void setConnectionRegistry(final ConnectionRegistry registry)
   {
      this.connectionRegistry = registry;
   }

   // Protected
   // ------------------------------------------------------------------------------------

   // Package Private
   // ------------------------------------------------------------------------------

   // Private
   // --------------------------------------------------------------------------------------

   private void handleFailover(final MessagingException me)
   {
      log.info("Connection failure has been detected, initiating failover");

      if (backupConnectorFactory == null)
      {
         throw new IllegalStateException("Cannot fail-over if backup connector factory is null");
      }

      for (ClientSessionInternal session : sessions)
      {
         // Need to get it once for each session to ensure ref count in
         // holder is
         // incremented properly
         RemotingConnection backupConnection = connectionRegistry.getConnection(backupConnectorFactory,
                                                                                backupTransportParams,
                                                                                pingPeriod,
                                                                                callTimeout);
         session.handleFailover(backupConnection);
      }

      this.connectorFactory = backupConnectorFactory;
      this.transportParams = backupTransportParams;

      this.backupConnectorFactory = null;
      this.backupTransportParams = null;

      failedOver = true;

      log.info("Failover complete");
   }

   private ConnectorFactory instantiateConnectorFactory(final String connectorFactoryClassName)
   {
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      try
      {
         Class<?> clazz = loader.loadClass(connectorFactoryClassName);
         return (ConnectorFactory) clazz.newInstance();
      }
      catch (Exception e)
      {
         throw new IllegalArgumentException("Error instantiating connector factory \"" + connectorFactoryClassName +
                                            "\"", e);
      }
   }

   private ClientSession createSessionInternal(final String username,
                                               final String password,
                                               final boolean xa,
                                               final boolean autoCommitSends,
                                               final boolean autoCommitAcks,
                                               int lazyAckBatchSize,
                                               boolean cacheProducers) throws MessagingException
   {
      Version clientVersion = VersionLoader.load();

      RemotingConnection connection = null;
      try
      {
         connection = connectionRegistry.getConnection(connectorFactory, transportParams, pingPeriod, callTimeout);

         if (backupConnectorFactory != null)
         {
            connection.addFailureListener(this);
         }

         String name = UUIDGenerator.getInstance().generateSimpleStringUUID().toString();

         long sessionChannelID = connection.generateChannelID();

         Packet request = new CreateSessionMessage(name,
                                                   sessionChannelID,
                                                   clientVersion.getIncrementingVersion(),
                                                   username,
                                                   password,
                                                   xa,
                                                   autoCommitSends,
                                                   autoCommitAcks);

         Channel channel1 = connection.getChannel(1, false, -1);

         Packet packet = channel1.sendBlocking(request);

         if (packet.getType() == EXCEPTION)
         {
            MessagingExceptionMessage mem = (MessagingExceptionMessage) packet;

            throw mem.getException();
         }

         CreateSessionResponseMessage response = (CreateSessionResponseMessage) packet;

         Channel sessionChannel = connection.getChannel(sessionChannelID,
                                                        false,
                                                        response.getPacketConfirmationBatchSize());

         ClientSessionInternal session = new ClientSessionImpl(this,
                                                               name,
                                                               xa,
                                                               lazyAckBatchSize,
                                                               cacheProducers,
                                                               autoCommitSends,
                                                               autoCommitAcks,
                                                               blockOnAcknowledge,
                                                               connection,
                                                               this,
                                                               response.getServerVersion(),
                                                               sessionChannel);

         sessions.add(session);

         ChannelHandler handler = new ClientSessionPacketHandler(session);

         sessionChannel.setHandler(handler);

         return session;
      }
      catch (Throwable t)
      {
         if (connection != null)
         {
            try
            {
               connectionRegistry.returnConnection(connection.getID());
            }
            catch (Throwable ignore)
            {
            }
         }

         if (t instanceof MessagingException)
         {
            throw (MessagingException) t;
         }
         else
         {
            MessagingException me = new MessagingException(MessagingException.INTERNAL_ERROR,
                                                           "Failed to start connection");

            me.initCause(t);

            throw me;
         }
      }
   }

   public void connectionFailed(final MessagingException me)
   {
      handleFailover(me);
   }

}
