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

package org.jboss.messaging.core.remoting.impl;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.CREATESESSION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.CREATESESSION_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.EARLY_RESPONSE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.EXCEPTION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.NULL_RESPONSE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.PACKETS_CONFIRMED;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.PING;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.PONG;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.REATTACH_SESSION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.REATTACH_SESSION_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.REPLICATE_CREATESESSION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.REPLICATION_RESPONSE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_ACKNOWLEDGE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_ADD_DESTINATION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_BINDINGQUERY;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_BINDINGQUERY_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_CHUNK_SEND;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_CLOSE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_COMMIT;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_CONSUMER_CLOSE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_CREATECONSUMER;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_CREATEQUEUE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_DELETE_QUEUE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_EXPIRED;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_FAILOVER_COMPLETE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_FLOWTOKEN;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_QUEUEQUERY;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_QUEUEQUERY_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_RECEIVE_MSG;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_REMOVE_DESTINATION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_REPLICATE_DELIVERY;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_ROLLBACK;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_SEND;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_START;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_STOP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_COMMIT;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_END;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_FORGET;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_GET_TIMEOUT;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_GET_TIMEOUT_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_INDOUBT_XIDS;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_INDOUBT_XIDS_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_JOIN;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_PREPARE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_RESUME;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_ROLLBACK;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_SET_TIMEOUT;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_SET_TIMEOUT_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_START;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_SUSPEND;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.ChannelHandler;
import org.jboss.messaging.core.remoting.DelayedResult;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.NullResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketsConfirmedMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;
import org.jboss.messaging.core.remoting.impl.wireformat.ReattachSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ReattachSessionResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ReplicateCreateSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAddDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCloseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionConsumerCloseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionConsumerFlowCreditMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionDeleteQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionExpiredMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionFailoverCompleteMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReceiveMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionRemoveDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReplicateDeliveryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionSendChunkMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionSendMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXACommitMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAEndMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAForgetMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAGetInDoubtXidsResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAGetTimeoutResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAJoinMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAPrepareMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAResumeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXARollbackMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXASetTimeoutMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXASetTimeoutResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAStartMessage;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.spi.Connector;
import org.jboss.messaging.core.remoting.spi.ConnectorFactory;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.util.SimpleIDGenerator;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @version <tt>$Revision$</tt> $Id$
 */
public class RemotingConnectionImpl extends AbstractBufferHandler implements RemotingConnection
{
   // Constants
   // ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(RemotingConnectionImpl.class);

   private static final float EXPIRE_FACTOR = 1.5f;

   // Static
   // ---------------------------------------------------------------------------------------

   public static RemotingConnection createConnection(final ConnectorFactory connectorFactory,
                                                     final Map<String, Object> params,
                                                     final long callTimeout,
                                                     final long pingInterval,
                                                     final ScheduledExecutorService pingExecutor,
                                                     final ConnectionLifeCycleListener listener)
   {
      DelegatingBufferHandler handler = new DelegatingBufferHandler();

      Connector connector = connectorFactory.createConnector(params, handler, listener);

      connector.start();

      Connection tc = connector.createConnection();

      if (tc == null)
      {
         throw new IllegalStateException("Failed to connect");
      }

      RemotingConnection connection = new RemotingConnectionImpl(tc, callTimeout, pingInterval, pingExecutor, null);

      handler.conn = connection;

      return connection;
   }

   private static class DelegatingBufferHandler extends AbstractBufferHandler
   {
      RemotingConnection conn;

      public void bufferReceived(final Object connectionID, final MessagingBuffer buffer)
      {
         conn.bufferReceived(connectionID, buffer);
      }
   }

   // Attributes
   // -----------------------------------------------------------------------------------

   private final Connection transportConnection;

   private final Map<Long, ChannelImpl> channels = new ConcurrentHashMap<Long, ChannelImpl>();

   private final List<FailureListener> failureListeners = new CopyOnWriteArrayList<FailureListener>();

   private final long blockingCallTimeout;

   private Runnable pinger;

   private final List<Interceptor> interceptors;

   private ScheduledFuture<?> future;

   private boolean firstTime = true;

   private volatile boolean gotPong;

   private volatile boolean destroyed;

   private long expirePeriod;

   private volatile boolean stopPinging;

   private volatile long expireTime = -1;

   private final Channel pingChannel;

   private final RemotingConnection replicatingConnection;

   private volatile boolean active;

   private final boolean client;

   private final long pingPeriod;

   private final ScheduledExecutorService pingExecutor;

   // Channels 0-9 are reserved for the system
   // 0 is for pinging
   // 1 is for session creation and attachment
   private volatile SimpleIDGenerator idGenerator = new SimpleIDGenerator(10);

   private boolean idGeneratorSynced = false;

   private final Object transferLock = new Object();

   private final ChannelHandler ppHandler = new PingPongHandler();

   private boolean frozen;

   private final Object failLock = new Object();

   // debug only stuff

   private boolean createdActive;

   // Constructors
   // ---------------------------------------------------------------------------------

   /*
    * Create a client side connection
    */
   public RemotingConnectionImpl(final Connection transportConnection,
                                 final long blockingCallTimeout,
                                 final long pingPeriod,
                                 final ScheduledExecutorService pingExecutor,
                                 final List<Interceptor> interceptors)
   {
      this(transportConnection, blockingCallTimeout, pingPeriod, pingExecutor, interceptors, null, true, true);
   }

   /*
    * Create a server side connection
    */
   public RemotingConnectionImpl(final Connection transportConnection,
                                 final List<Interceptor> interceptors,
                                 final RemotingConnection replicatingConnection,
                                 final boolean active)

   {
      this(transportConnection, -1, -1, null, interceptors, replicatingConnection, active, false);
   }

   private RemotingConnectionImpl(final Connection transportConnection,
                                  final long blockingCallTimeout,
                                  final long pingPeriod,
                                  final ScheduledExecutorService pingExecutor,
                                  final List<Interceptor> interceptors,
                                  final RemotingConnection replicatingConnection,
                                  final boolean active,
                                  final boolean client)

   {
      this.transportConnection = transportConnection;

      this.blockingCallTimeout = blockingCallTimeout;

      this.interceptors = interceptors;

      this.replicatingConnection = replicatingConnection;

      if (replicatingConnection != null)
      {
         replicatingConnection.addFailureListener(new ReplicatingConnectionFailureListener());
      }

      this.active = active;

      this.pingPeriod = pingPeriod;

      this.pingExecutor = pingExecutor;

      // Channel zero is reserved for pinging
      pingChannel = getChannel(0, -1, false);

      pingChannel.setHandler(ppHandler);

      this.client = client;

      this.createdActive = active;
   }

   public void startPinger()
   {
      if (pingPeriod != -1)
      {
         pinger = new Pinger();

         expirePeriod = (long)(EXPIRE_FACTOR * pingPeriod);

         future = pingExecutor.scheduleWithFixedDelay(pinger, 0, pingPeriod, TimeUnit.MILLISECONDS);
      }
      else
      {
         pinger = null;
      }
   }

   // RemotingConnection implementation
   // ------------------------------------------------------------

   public Object getID()
   {
      return transportConnection.getID();
   }

   public String getRemoteAddress()
   {
      return transportConnection.getRemoteAddress();
   }

   public synchronized Channel getChannel(final long channelID, final int windowSize, final boolean block)
   {
      ChannelImpl channel = channels.get(channelID);

      if (channel == null)
      {
         channel = new ChannelImpl(this, channelID, windowSize, block);

         channels.put(channelID, channel);
      }

      return channel;
   }

   public void addFailureListener(final FailureListener listener)
   {
      if (listener == null)
      {
         throw new IllegalStateException("FailureListener cannot be null");
      }

      failureListeners.add(listener);
   }

   public boolean removeFailureListener(final FailureListener listener)
   {
      if (listener == null)
      {
         throw new IllegalStateException("FailureListener cannot be null");
      }

      return failureListeners.remove(listener);
   }

   public MessagingBuffer createBuffer(final int size)
   {
      return transportConnection.createBuffer(size);
   }

   public RemotingConnection getReplicatingConnection()
   {
      return replicatingConnection;
   }

   /*
    * This can be called concurrently by more than one thread so needs to be locked
    */
   public void fail(final MessagingException me)
   {
      synchronized (failLock)
      {
         if (destroyed)
         {
            return;
         }

         log.warn(me.getMessage());

         // Then call the listeners
         callListeners(me);

         internalClose();

         for (Channel channel : channels.values())
         {
            channel.fail();
         }
      }
   }

   public void destroy()
   {
      synchronized (failLock)
      {
         if (destroyed)
         {
            return;
         }

         internalClose();

         // TODO: https://jira.jboss.org/jira/browse/JBMESSAGING-1421
         // This affects clustering, so I'm keeping this out for now
         // We need to inform Listeners about the connection being closed
         // callListeners(null);
      }
   }

   public boolean isExpired(final long now)
   {
      return expireTime != -1 && now >= expireTime;
   }

   public long generateChannelID()
   {
      return idGenerator.generateID();
   }

   /* For testing only */
   public void stopPingingAfterOne()
   {
      stopPinging = true;
   }

   public synchronized void syncIDGeneratorSequence(final long id)
   {
      if (!idGeneratorSynced)
      {
         idGenerator = new SimpleIDGenerator(id);

         idGeneratorSynced = true;
      }
   }

   public long getIDGeneratorSequence()
   {
      return idGenerator.getCurrentID();
   }

   // Buffer Handler implementation
   // ----------------------------------------------------

   public void bufferReceived(final Object connectionID, final MessagingBuffer buffer)
   {
      final Packet packet = decode(buffer);

      synchronized (transferLock)
      {
         if (!frozen)
         {
            final ChannelImpl channel = channels.get(packet.getChannelID());

            if (channel != null)
            {
               channel.handlePacket(packet);
            }
         }
      }
   }

   public void activate()
   {
      active = true;
   }

   public void freeze()
   {
      // Prevent any more packets being handled on this connection

      synchronized (transferLock)
      {
         frozen = true;
      }
   }

   // Package protected
   // ----------------------------------------------------------------------------

   // Protected
   // ------------------------------------------------------------------------------------

   // Private
   // --------------------------------------------------------------------------------------

   private void callListeners(final MessagingException me)
   {
      final Set<FailureListener> listenersClone = new HashSet<FailureListener>(failureListeners);

      for (final FailureListener listener : listenersClone)
      {
         try
         {
            listener.connectionFailed(me);
         }
         catch (final Throwable t)
         {
            // Failure of one listener to execute shouldn't prevent others
            // from
            // executing
            log.error("Failed to execute failure listener", t);
         }
      }
   }

   private void internalClose()
   {
      if (future != null)
      {
         future.cancel(false);
      }

      pingChannel.close();

      destroyed = true;

      // We close the underlying transport connection
      transportConnection.close();

      if (replicatingConnection != null)
      {
         replicatingConnection.destroy();
      }

      for (Channel channel : channels.values())
      {
         channel.close();
      }
   }

   private Packet decode(final MessagingBuffer in)
   {
      final byte packetType = in.getByte();

      Packet packet;

      switch (packetType)
      {
         case PING:
         {
            packet = new Ping();
            break;
         }
         case PONG:
         {
            packet = new PacketImpl(PacketImpl.PONG);
            break;
         }
         case EXCEPTION:
         {
            packet = new MessagingExceptionMessage();
            break;
         }
         case PACKETS_CONFIRMED:
         {
            packet = new PacketsConfirmedMessage();
            break;
         }
         case REPLICATION_RESPONSE:
         {
            packet = new PacketImpl(REPLICATION_RESPONSE);
            break;
         }
         case CREATESESSION:
         {
            packet = new CreateSessionMessage();
            break;
         }
         case REPLICATE_CREATESESSION:
         {
            packet = new ReplicateCreateSessionMessage();
            break;
         }
         case CREATESESSION_RESP:
         {
            packet = new CreateSessionResponseMessage();
            break;
         }
         case REATTACH_SESSION:
         {
            packet = new ReattachSessionMessage();
            break;
         }
         case REATTACH_SESSION_RESP:
         {
            packet = new ReattachSessionResponseMessage();
            break;
         }
         case SESS_FAILOVER_COMPLETE:
         {
            packet = new SessionFailoverCompleteMessage();
            break;
         }
         case SESS_CLOSE:
         {
            packet = new SessionCloseMessage();
            break;
         }
         case SESS_CREATECONSUMER:
         {
            packet = new SessionCreateConsumerMessage();
            break;
         }
         case SESS_ACKNOWLEDGE:
         {
            packet = new SessionAcknowledgeMessage();
            break;
         }
         case SESS_EXPIRED:
         {
            packet = new SessionExpiredMessage();
            break;
         }
         case SESS_COMMIT:
         {
            packet = new PacketImpl(PacketImpl.SESS_COMMIT);
            break;
         }
         case SESS_ROLLBACK:
         {
            packet = new PacketImpl(PacketImpl.SESS_ROLLBACK);
            break;
         }
         case SESS_QUEUEQUERY:
         {
            packet = new SessionQueueQueryMessage();
            break;
         }
         case SESS_QUEUEQUERY_RESP:
         {
            packet = new SessionQueueQueryResponseMessage();
            break;
         }
         case SESS_CREATEQUEUE:
         {
            packet = new SessionCreateQueueMessage();
            break;
         }
         case SESS_DELETE_QUEUE:
         {
            packet = new SessionDeleteQueueMessage();
            break;
         }
         case SESS_ADD_DESTINATION:
         {
            packet = new SessionAddDestinationMessage();
            break;
         }
         case SESS_REMOVE_DESTINATION:
         {
            packet = new SessionRemoveDestinationMessage();
            break;
         }
         case SESS_BINDINGQUERY:
         {
            packet = new SessionBindingQueryMessage();
            break;
         }
         case SESS_BINDINGQUERY_RESP:
         {
            packet = new SessionBindingQueryResponseMessage();
            break;
         }
         case SESS_XA_START:
         {
            packet = new SessionXAStartMessage();
            break;
         }
         case SESS_XA_END:
         {
            packet = new SessionXAEndMessage();
            break;
         }
         case SESS_XA_COMMIT:
         {
            packet = new SessionXACommitMessage();
            break;
         }
         case SESS_XA_PREPARE:
         {
            packet = new SessionXAPrepareMessage();
            break;
         }
         case SESS_XA_RESP:
         {
            packet = new SessionXAResponseMessage();
            break;
         }
         case SESS_XA_ROLLBACK:
         {
            packet = new SessionXARollbackMessage();
            break;
         }
         case SESS_XA_JOIN:
         {
            packet = new SessionXAJoinMessage();
            break;
         }
         case SESS_XA_SUSPEND:
         {
            packet = new PacketImpl(PacketImpl.SESS_XA_SUSPEND);
            break;
         }
         case SESS_XA_RESUME:
         {
            packet = new SessionXAResumeMessage();
            break;
         }
         case SESS_XA_FORGET:
         {
            packet = new SessionXAForgetMessage();
            break;
         }
         case SESS_XA_INDOUBT_XIDS:
         {
            packet = new PacketImpl(PacketImpl.SESS_XA_INDOUBT_XIDS);
            break;
         }
         case SESS_XA_INDOUBT_XIDS_RESP:
         {
            packet = new SessionXAGetInDoubtXidsResponseMessage();
            break;
         }
         case SESS_XA_SET_TIMEOUT:
         {
            packet = new SessionXASetTimeoutMessage();
            break;
         }
         case SESS_XA_SET_TIMEOUT_RESP:
         {
            packet = new SessionXASetTimeoutResponseMessage();
            break;
         }
         case SESS_XA_GET_TIMEOUT:
         {
            packet = new PacketImpl(PacketImpl.SESS_XA_GET_TIMEOUT);
            break;
         }
         case SESS_XA_GET_TIMEOUT_RESP:
         {
            packet = new SessionXAGetTimeoutResponseMessage();
            break;
         }
         case SESS_START:
         {
            packet = new PacketImpl(PacketImpl.SESS_START);
            break;
         }
         case SESS_STOP:
         {
            packet = new PacketImpl(PacketImpl.SESS_STOP);
            break;
         }
         case SESS_FLOWTOKEN:
         {
            packet = new SessionConsumerFlowCreditMessage();
            break;
         }
         case SESS_SEND:
         {
            packet = new SessionSendMessage();
            break;
         }
         case SESS_RECEIVE_MSG:
         {
            packet = new SessionReceiveMessage();
            break;
         }
         case SESS_CONSUMER_CLOSE:
         {
            packet = new SessionConsumerCloseMessage();
            break;
         }
         case NULL_RESPONSE:
         {
            packet = new NullResponseMessage();
            break;
         }
         case SESS_CHUNK_SEND:
         {
            packet = new SessionSendChunkMessage();
            break;
         }
         case SESS_REPLICATE_DELIVERY:
         {
            packet = new SessionReplicateDeliveryMessage();
            break;
         }
         default:
         {
            throw new IllegalArgumentException("Invalid type: " + packetType);
         }
      }

      packet.decode(in);

      return packet;
   }

   // Inner classes
   // --------------------------------------------------------------------------------

   // Needs to be static so we can re-assign it to another remotingconnection
   private static class ChannelImpl implements Channel
   {
      private final long id;

      private ChannelHandler handler;

      private Packet response;

      private final java.util.Queue<Packet> resendCache;

      private volatile int firstStoredCommandID;

      private volatile int lastReceivedCommandID = -1;

      private Channel replicatingChannel;

      private volatile RemotingConnectionImpl connection;

      private volatile boolean closed;

      private final Lock lock = new ReentrantLock();

      private final Condition sendCondition = lock.newCondition();

      private final Condition failoverCondition = lock.newCondition();

      private final Object sendLock = new Object();

      private final Object sendBlockingLock = new Object();

      private final Object replicationLock = new Object();

      private boolean failingOver;

      private final Queue<DelayedResult> responseActions = new ConcurrentLinkedQueue<DelayedResult>();

      private final int windowSize;

      private final int confWindowSize;

      private final Semaphore sendSemaphore;

      private int receivedBytes;

      private ChannelImpl(final RemotingConnectionImpl connection,
                          final long id,
                          final int windowSize,
                          final boolean block)
      {
         this.connection = connection;

         this.id = id;

         if (connection.replicatingConnection != null && id != 0)
         {
            // We don't redirect the ping channel

            replicatingChannel = connection.replicatingConnection.getChannel(id, -1, false);

            replicatingChannel.setHandler(new ReplicatedPacketsConfirmedChannelHandler());
         }
         this.windowSize = windowSize;

         this.confWindowSize = (int)(0.75 * windowSize);

         if (this.windowSize != -1)
         {
            resendCache = new ConcurrentLinkedQueue<Packet>();

            if (block)
            {
               sendSemaphore = new Semaphore(windowSize, true);
            }
            else
            {
               sendSemaphore = null;
            }
         }
         else
         {
            resendCache = null;

            sendSemaphore = null;
         }
      }

      public long getID()
      {
         return id;
      }

      public int getLastReceivedCommandID()
      {
         return lastReceivedCommandID;
      }

      public Lock getLock()
      {
         return lock;
      }

      public void returnBlocking()
      {
         lock.lock();

         try
         {
            response = new PacketImpl(EARLY_RESPONSE);

            sendCondition.signal();
         }
         finally
         {
            lock.unlock();
         }
      }

      // This must never called by more than one thread concurrently
      public void send(final Packet packet)
      {
         synchronized (sendLock)
         {
            packet.setChannelID(id);

            final MessagingBuffer buffer = connection.transportConnection.createBuffer(packet.getRequiredBufferSize());

            int size = packet.encode(buffer);

            // Must block on semaphore outside the main lock or this can prevent failover from occurring
            if (sendSemaphore != null && packet.getType() != PACKETS_CONFIRMED)
            {
               try
               {
                  sendSemaphore.acquire(size);
               }
               catch (InterruptedException e)
               {
                  throw new IllegalStateException("Semaphore interrupted");
               }
            }

            lock.lock();

            try
            {
               while (failingOver)
               {
                  // TODO - don't hardcode this timeout
                  try
                  {
                     failoverCondition.await(10000, TimeUnit.MILLISECONDS);
                  }
                  catch (InterruptedException e)
                  {
                  }
               }

               if (resendCache != null && packet.isRequiresConfirmations())
               {
                  resendCache.add(packet);
               }

               if (connection.active || packet.isWriteAlways())
               {
                  connection.transportConnection.write(buffer);
               }
            }
            finally
            {
               lock.unlock();
            }
         }
      }

      public Packet sendBlocking(final Packet packet) throws MessagingException
      {         
         // System.identityHashCode(this.connection) + " " + packet.getType());

         if (closed)
         {            
            throw new MessagingException(MessagingException.NOT_CONNECTED, "Connection is destroyed");
         }

         if (connection.blockingCallTimeout == -1)
         {
            throw new IllegalStateException("Cannot do a blocking call timeout on a server side connection");
         }

         // Synchronized since can't be called concurrently by more than one thread and this can occur
         // E.g. blocking acknowledge() from inside a message handler at some time as other operation on main thread
         synchronized (sendBlockingLock)
         {
            packet.setChannelID(id);

            final MessagingBuffer buffer = connection.transportConnection.createBuffer(packet.getRequiredBufferSize());

            int size = packet.encode(buffer);

            // Must block on semaphore outside the main lock or this can prevent failover from occurring
            if (sendSemaphore != null)
            {
               try
               {
                  sendSemaphore.acquire(size);
               }
               catch (InterruptedException e)
               {
                  throw new IllegalStateException("Semaphore interrupted");
               }
            }

            lock.lock();
            
            try
            {
               while (failingOver)
               {
                  // TODO - don't hardcode this timeout
                  try
                  {
                     failoverCondition.await(10000, TimeUnit.MILLISECONDS);
                  }
                  catch (InterruptedException e)
                  {
                  }
               }

               response = null;

               if (resendCache != null && packet.isRequiresConfirmations())
               {
                  resendCache.add(packet);
               }

               connection.transportConnection.write(buffer);

               long toWait = connection.blockingCallTimeout;

               long start = System.currentTimeMillis();

               while (response == null && toWait > 0)
               {
                  try
                  {
                     sendCondition.await(toWait, TimeUnit.MILLISECONDS);
                  }
                  catch (InterruptedException e)
                  {
                  }

                  final long now = System.currentTimeMillis();

                  toWait -= now - start;

                  start = now;
               }

               if (response == null)
               {
                  throw new MessagingException(MessagingException.CONNECTION_TIMEDOUT,
                                               "Timed out waiting for response when sending packet " + packet.getType());
               }

               if (response.getType() == PacketImpl.EXCEPTION)
               {
                  final MessagingExceptionMessage mem = (MessagingExceptionMessage)response;

                  throw mem.getException();
               }
               else
               {
                  return response;
               }
            }
            finally
            {
               lock.unlock();
            }
         }
      }

      // Must be synchronized since can be called by incoming session commands but also by deliveries
      // Also needs to be synchronized with respect to replicatingChannelDead
      public DelayedResult replicatePacket(final Packet packet)
      {
         synchronized (replicationLock)
         {
            if (replicatingChannel != null)
            {
               DelayedResult result = new DelayedResult();

               responseActions.add(result);

               replicatingChannel.send(packet);

               return result;
            }
            else
            {
               return null;
            }
         }
      }

      // The replicating connection has died (backup has died)
      public void replicatingChannelDead()
      {
         synchronized (replicationLock)
         {
            replicatingChannel = null;

            // Execute all the response actions now

            while (true)
            {
               DelayedResult result = responseActions.poll();

               if (result != null)
               {
                  result.replicated();
               }
               else
               {
                  break;
               }
            }
         }
      }

      public void replicateComplete()
      {
         if (!connection.active)
         {
            // We're on backup so send back a replication response

            Packet packet = new PacketImpl(REPLICATION_RESPONSE);

            packet.setChannelID(id);

            doWrite(packet);
         }
      }

      // This will never get called concurrently by more than one thread

      // TODO it's not ideal synchronizing this since it forms a contention point with replication
      // but we need to do this to protect it w.r.t. the check on replicatingChannel
      public void replicateResponseReceived()
      {
         DelayedResult result = null;

         synchronized (replicationLock)
         {
            if (replicatingChannel != null)
            {
               result = responseActions.poll();

               if (result == null)
               {
                  throw new IllegalStateException("Cannot find response action");
               }
            }
         }

         // Must execute outside of lock
         if (result != null)
         {
            result.replicated();
         }
      }

      public void setHandler(final ChannelHandler handler)
      {
         this.handler = handler;
      }

      public void close()
      {         
         if (closed)
         {
            return;
         }

         if (!connection.destroyed && connection.channels.remove(id) == null)
         {
            throw new IllegalArgumentException("Cannot find channel with id " + id + " to close");
         }

         if (replicatingChannel != null)
         {
            replicatingChannel.close();
         }

         closed = true;
      }

      public void fail()
      {
      }

      public Channel getReplicatingChannel()
      {
         return replicatingChannel;
      }

      public void transferConnection(final RemotingConnection newConnection)
      {
         // Needs to synchronize on the connection to make sure no packets from
         // the old connection get processed after transfer has occurred
         synchronized (connection.transferLock)
         {
            connection.channels.remove(id);

            // And switch it

            final RemotingConnectionImpl rnewConnection = (RemotingConnectionImpl)newConnection;

            if (rnewConnection.channels.containsKey(id))
            {
               throw new IllegalStateException("Backup connection already has channel with id " + id);
            }

            rnewConnection.channels.put(id, this);

            connection = rnewConnection;
         }
      }

      public void replayCommands(final int otherLastReceivedCommandID)
      {
         clearUpTo(otherLastReceivedCommandID);

         for (final Packet packet : resendCache)
         {
            doWrite(packet);
         }
      }

      public void lock()
      {
         lock.lock();

         failingOver = true;

         lock.unlock();
      }

      public void unlock()
      {
         lock.lock();

         failingOver = false;

         failoverCondition.signalAll();

         lock.unlock();
      }

      public RemotingConnection getConnection()
      {
         return connection;
      }

      private void handlePacket(final Packet packet)
      {
         if (packet.getType() == PACKETS_CONFIRMED)
         {
            if (resendCache != null)
            {
               final PacketsConfirmedMessage msg = (PacketsConfirmedMessage)packet;

               clearUpTo(msg.getCommandID());
            }
            else if (replicatingChannel != null)
            {
               replicatingChannel.send(packet);
            }
            else
            {
               handler.handlePacket(packet);
            }

            return;
         }
         else
         {
            if (connection.interceptors != null)
            {
               for (final Interceptor interceptor : connection.interceptors)
               {
                  try
                  {
                     boolean callNext = interceptor.intercept(packet, connection);

                     if (!callNext)
                     {
                        // abort

                        return;
                     }
                  }
                  catch (final Throwable e)
                  {
                     log.warn("Failure in calling interceptor: " + interceptor, e);
                  }
               }
            }

            if (packet.isResponse())
            {
               response = packet;

               confirm(packet);

               lock.lock();

               try
               {
                  sendCondition.signal();
               }
               finally
               {
                  lock.unlock();
               }
            }
            else if (handler != null)
            {
               handler.handlePacket(packet);
            }
         }
      }

      private void doWrite(final Packet packet)
      {
         final MessagingBuffer buffer = connection.transportConnection.createBuffer(packet.getRequiredBufferSize());

         packet.encode(buffer);

         connection.transportConnection.write(buffer);
      }

      public void confirm(final Packet packet)
      {
         if (resendCache != null && packet.isRequiresConfirmations())
         {
            lastReceivedCommandID++;

            receivedBytes += packet.getPacketSize();

            if (receivedBytes >= confWindowSize)
            {
               receivedBytes = 0;

               if (connection.active)
               {
                  final Packet confirmed = new PacketsConfirmedMessage(lastReceivedCommandID);

                  confirmed.setChannelID(id);

                  doWrite(confirmed);
               }
            }
         }
      }

      private void clearUpTo(final int lastReceivedCommandID)
      {
         final int numberToClear = 1 + lastReceivedCommandID - firstStoredCommandID;

         if (numberToClear == -1)
         {
            throw new IllegalArgumentException("Invalid lastReceivedCommandID: " + lastReceivedCommandID);
         }

         int sizeToFree = 0;

         for (int i = 0; i < numberToClear; i++)
         {
            final Packet packet = resendCache.poll();

            if (packet == null)
            {
               throw new IllegalStateException(System.identityHashCode(this) + " Can't find packet to clear: " +
                                               " last received command id " +
                                               lastReceivedCommandID +
                                               " first stored command id " +
                                               firstStoredCommandID +
                                               " cache size " +
                                               this.resendCache.size() +
                                               " channel id " +
                                               id +
                                               " client " +
                                               connection.client +
                                               " created active " +
                                               connection.createdActive);
            }

            if (packet.getType() != PACKETS_CONFIRMED)
            {
               sizeToFree += packet.getPacketSize();
            }
         }

         firstStoredCommandID += numberToClear;

         if (sendSemaphore != null)
         {
            sendSemaphore.release(sizeToFree);
         }
      }

      private class ReplicatedPacketsConfirmedChannelHandler implements ChannelHandler
      {
         public void handlePacket(final Packet packet)
         {
            switch (packet.getType())
            {
               case REPLICATION_RESPONSE:
               {
                  replicateResponseReceived();

                  break;
               }
               default:
               {
                  throw new IllegalArgumentException("Invalid packet " + packet);
               }
            }
         }
      }
   }

   private class Pinger implements Runnable
   {
      public synchronized void run()
      {
         if (!firstTime && !gotPong)
         {
            // Error - didn't get pong back
            final MessagingException me = new MessagingException(MessagingException.NOT_CONNECTED,
                                                                 "Did not receive pong from server, active " + createdActive +
                                                                          " client " +
                                                                          client);

            fail(me);
         }

         gotPong = false;

         firstTime = false;

         // Send ping
         final Packet ping = new Ping(expirePeriod);

         pingChannel.send(ping);
      }
   }

   private class PingPongHandler implements ChannelHandler
   {
      public void handlePacket(final Packet packet)
      {
         final byte type = packet.getType();

         if (type == PONG)
         {
            gotPong = true;

            if (stopPinging)
            {
               future.cancel(true);
            }
         }
         else if (type == PING)
         {
            expireTime = System.currentTimeMillis() + ((Ping)packet).getExpirePeriod();

            // Parameter is placeholder for future
            final Packet pong = new Pong(-1);

            pingChannel.send(pong);
         }
         else
         {
            throw new IllegalArgumentException("Invalid packet: " + packet);
         }
      }
   }

   private class ReplicatingConnectionFailureListener implements FailureListener
   {
      public void connectionFailed(final MessagingException me)
      {
         synchronized (RemotingConnectionImpl.this)
         {
            for (Channel channel : channels.values())
            {
               channel.replicatingChannelDead();
            }
         }
      }
   }
}
